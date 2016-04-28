/**
 * collectd - src/mapr_process.c
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Naveen Tirupattur <ntirupattur@maprtech.com>
 **/

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"
#include "stdio.h"
#include "sys/types.h"
#include "dirent.h"



# include <glob.h>
# include <sys/stat.h>
# include <linux/limits.h>
# include <sys/sysinfo.h>
# if HAVE_LINUX_CONFIG_H
#   include <linux/config.h>
# endif
# define PSCMD  "ps -eo uid,pid,ppid,pgid,args"
# define PSFORMAT   "%ld %ld %ld %ld %[^\n]"
# define PSVARS &P[i].uid, &P[i].pid, &P[i].ppid, &P[i].pgid, P[i].cmd
# define PSVARSN  5
# define CMDLINE_BUFFER_SIZE 4096
# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <unistd.h>    /* For getopt() */
# include <pwd.h>   /* For getpwnam() */
# include <sys/ioctl.h>   /* For TIOCGSIZE/TIOCGWINSZ */
#include <sys/times.h>
#include <time.h>

#ifndef TRUE
#  define TRUE  1
#  define FALSE 0
#endif
# define MAXLINE 8192

# include <dirent.h>
# include <regex.h>

struct Proc {
  long uid, pid, ppid, pgid;
  char name[32], cmd[MAXLINE];
  int print;
  long parent, child, sister;
  unsigned long thcount;
}*P;

//static void uid2user(uid_t uid, char *name, int len) {
//#define NUMUN 128
//  static struct un_ {
//    uid_t uid;
//    char name[32];
//  }un[NUMUN];
//  static short n = 0;
//  short i;
//  char uid_name[32];
//  char *found;
//#ifdef DEBUG
//  if (name == NULL) {
//    for (i = 0; i < n; i++)
//      fprintf(stderr, "uid = %3d, name = %s\n", un[i].uid, un[i].name);
//    return;
//  }
//#endif
//  for (i = n - 1; i >= 0 && un[i].uid != uid; i--);
//  if (i >= 0) { /* found locally */
//    found = un[i].name;
//  } else {
//    struct passwd *pw = getpwuid(uid);
//    if (pw) {
//      found = pw->pw_name;
//    } else {
//      /* fix by Stan Sieler & Philippe Torche */
//      snprintf(uid_name, sizeof(uid_name), "#%d", uid);
//      found = uid_name;
//    }
//    if (n < NUMUN) {
//      un[n].uid = uid;
//      strncpy(un[n].name, found, 9);
//      un[n].name[8] = '\0';
//      n++;
//    }
//  }
//  strncpy(name, found, len);
//  name[len-1] = '\0';
//}
#define PROCSTAT_NAME_LEN 256

typedef struct procstat {
  char name[PROCSTAT_NAME_LEN];
  char processName[PROCSTAT_NAME_LEN];
  unsigned long pid;
  unsigned long ppid;
  unsigned long starttime_secs;
  unsigned long runtime_secs;

  unsigned long num_proc;
  unsigned long num_lwp;
  unsigned long vmem_size;
  unsigned long vmem_rss;
  unsigned long vmem_data;
  unsigned long vmem_code;
  unsigned long stack_size;

  derive_t vmem_minflt_counter;
  derive_t vmem_majflt_counter;

  derive_t cpu_user_counter;
  derive_t cpu_system_counter;
  derive_t cpu_child_user_counter;
  derive_t cpu_child_system_counter;

  double cpu_percent;
  float mem_percent;

  /* io data */
  derive_t io_rchar;
  derive_t io_wchar;
  derive_t io_syscr;
  derive_t io_syscw;

  derive_t cswitch_vol;
  derive_t cswitch_invol;

  struct procstat *next;
} procstat_t;

typedef struct sysstat {
  unsigned long sys_cpu_user_counter; /* /proc/stat */
  unsigned long sys_cpu_system_counter; /* /proc/stat */
  unsigned long sys_cpu_tot_time_counter; /* /proc/stat */
  unsigned long long sys_tot_phys_mem; /* /proc/meminfo */
  unsigned long sys_boot_time_secs; /* /proc/stat */
} sysstat_t;

typedef struct directorylist {
  char directoryName[PATH_MAX];
  struct directorylist *next;
} directorylist_t;

static procstat_t *list_head_g = NULL;
static procstat_t *proc_list_head_g = NULL;
static procstat_t *prev_proc_list_head_g = NULL;
static directorylist_t *directory_list_head_g = NULL;

/* configuration globals */
static float filter_mincpupct_g = 0.0;
static float filter_minmempct_g = 0.0;
//static int numOfProcesses = 0;

static long pagesize_g;
static long clockTicks;
static int numCores;


/* Read /proc/ */
//static int getProcesses(void) {
//  glob_t globbuf;
//  unsigned int i, j;
//
//  glob("/proc/[0-9]*", GLOB_NOSORT, NULL, &globbuf);
//
//  P = calloc(globbuf.gl_pathc, sizeof(struct Proc));
//  if (P == NULL) {
//    fprintf(stderr, "Problems with malloc.\n");
//    exit(1);
//  }
//
//  for (i = j = 0; i < globbuf.gl_pathc; i++) {
//    char *pdir, name[32];
//    int c;
//    FILE *processFP;
//    int k = 0;
//
//    pdir = globbuf.gl_pathv[globbuf.gl_pathc - i - 1];
//
//    /* if processes change their UID this change is only reflected in the owner of pdir.
//     * fixed since version 2.36 */
//    {
//      struct stat st;
//      if (stat(pdir, &st) != 0) { /* get uid */
//        continue; /* process vanished since glob() */
//      }
//      P[j].uid = st.st_uid;
//      uid2user(P[j].uid, P[j].name, sizeof(P[j].name));
//    }
//
//    snprintf(name, sizeof(name), "%s%s",
//        globbuf.gl_pathv[globbuf.gl_pathc - i - 1], "/stat");
//    processFP = fopen(name, "r");
//    if (processFP == NULL)
//      continue; /* process vanished since glob() */
//    int status = fscanf(processFP, "%ld %s %*c %ld %ld", &P[j].pid, P[j].cmd, &P[j].ppid,
//        &P[j].pgid);
//    if (status == 0) {
//      ERROR("mapr_process plugin: Failed to read from /proc/pid/stat.");
//      continue;
//    }
//    fclose(processFP);
//    P[j].thcount = 1;
//
//    snprintf(name, sizeof(name), "%s%s",
//        globbuf.gl_pathv[globbuf.gl_pathc - i - 1], "/cmdline");
//    processFP = fopen(name, "r");
//    if (processFP == NULL)
//      continue; /* process vanished since glob() */
//    while (k < MAXLINE - 1 && EOF != (c = fgetc(processFP))) {
//      P[j].cmd[k++] = c == '\0' ? ' ' : c;
//    }
//    if (k > 0)
//      P[j].cmd[k] = '\0';
//    fclose(processFP);
//    P[j].parent = P[j].child = P[j].sister = -1;
//    j++;
//  }
//  globfree(&globbuf);
//  return j;
//} /* int getProcesses() */
//
//int get_pid_index(long pid) {
//  int me;
//  for (me = numOfProcesses - 1; me >= 0 && P[me].pid != pid; me--)
//    ; /* Search process */
//  return me;
//}
//
//#define EXIST(idx) ((idx) != -1)

//static void MakeTree(void) {
//  /* Build the process hierarchy. Every process marks itself as first child
//   * of it's parent or as sister of first child of it's parent */
//  int me;
//  for (me = 0; me < numOfProcesses; me++) {
//    int parent;
//    parent = get_pid_index(P[me].ppid);
//    if (parent != me && parent != -1) { /* valid process, not me */
//      P[me].parent = parent;
//      if (P[parent].child == -1) /* first child */
//        P[parent].child = me;
//      else {
//        int sister;
//        for (sister = P[parent].child; EXIST(P[sister].sister); sister =
//            P[sister].sister)
//          ;
//        P[sister].sister = me;
//      }
//    }
//  }
//}

/* put name of process from config to list_head_g tree
 * list_head_g is a list of 'procstat_t' structs with
 * processes names we want to watch */
static void ps_list_register(int pid, char *name) {
  procstat_t *new;
  procstat_t *ptr;

  new = (procstat_t *) malloc(sizeof(procstat_t));
  if (new == NULL) {
    ERROR("mapr_process plugin: ps_list_register: malloc failed.");
    return;
  }
  memset(new, 0, sizeof(procstat_t));
  // Remove .pid from file name
  char *serviceName = strtok(name,".");
  sstrncpy(new->processName, serviceName, sizeof(new->processName));
  new->pid = pid;

  for (ptr = list_head_g; ptr != NULL; ptr = ptr->next) {
    //PID is already in the list
    if (ptr->pid == pid && strcmp(serviceName, ptr->processName) == 0) {
      WARNING ("Found an entry for pid %d for service %s in the list",pid, serviceName);
      sfree(new);
      return;
    }
    if (ptr->next == NULL)
      break;
  }

  if (ptr == NULL) {
    list_head_g = new;
  } else {
    ptr->next = new;
  }
}/* void ps_list_register */

/*
 * get all the pids in this directory
 */
static void getPids(char *name) {
  DIR *directory;
  struct dirent *directoryEntry;
  FILE *pidFP;
  int pid = -1;
  directory = opendir(name);
  if (directory != NULL) {
    while ((directoryEntry = readdir(directory))) {
      if (!strcmp (directoryEntry->d_name, "."))
        continue;
      if (!strcmp (directoryEntry->d_name, ".."))
        continue;
      char fileName[PATH_MAX];
      memset(fileName, '\0', sizeof(fileName));
      strcpy(fileName,name);
      strcat(fileName,"/");
      strcat(fileName,directoryEntry->d_name);
      int filename_length = strlen(fileName);
      if (filename_length >= 4 && strcmp(fileName + filename_length - 4, ".pid") == 0 && !strcmp(fileName + filename_length - 7, ".sh.pid") == 0) {
        pidFP = fopen(fileName, "r");
        if (pidFP == NULL) {
          ERROR("mapr_process plugin failed to open pid file %s", directoryEntry->d_name);
        } else {
          int status = fscanf(pidFP, "%d", &pid);
          if ( status == 0 ) {
            ERROR("mapr_process plugin failed to read pid file %s", directoryEntry->d_name);
            continue;
          }
          ps_list_register(pid, directoryEntry->d_name);
          fclose(pidFP);
        }
      }
    }
    closedir(directory);
  }
} /* void getPids */

static void ps_proc_list_prepend(procstat_t *ps)
{
  if (proc_list_head_g == NULL) {
    proc_list_head_g = (procstat_t *)malloc(sizeof(procstat_t));
    if (proc_list_head_g == NULL) {
      ERROR ("mapr_process plugin: error allocating memory");
      return;
    }
    ps->next = NULL;
    memcpy(proc_list_head_g, ps, sizeof(procstat_t));
  }
  else {
    procstat_t *new;
    new = (procstat_t *)malloc(sizeof(procstat_t));
    if (new == NULL) {
      ERROR ("mapr_process plugin: error allocating memory");
      return;
    }
    memcpy(new, ps, sizeof(procstat_t));
    new->next = proc_list_head_g;
    proc_list_head_g = new;
  }
} /* void ps_proc_list_prepend */

static void ps_proc_list_reset (procstat_t **head)
{
  procstat_t *ps;

  ps = *head;
  while (ps) {
    procstat_t *nps;
    nps = ps->next;
    free(ps);
    ps = nps;
  }
  *head = NULL;
} /* void ps_proc_list_reset */

/* put all pre-defined 'Process' names from config to list_head_g tree */
static int ps_config(oconfig_item_t *ci) {
  int i;
  directorylist_t *dirlist;
  for (i = 0; i < ci->children_num; ++i) {
    oconfig_item_t *c = ci->children + i;
    if (strcasecmp(c->key, "MinCPUPercent") == 0) {
      filter_mincpupct_g = c->values[0].value.number;
      if (filter_mincpupct_g < 0.0 || filter_mincpupct_g > 100.0) {
        ERROR("mapr_process plugin: MinCPUPercent out of [0,100] range");
        continue;
      }
    } else if (strcasecmp(c->key, "MinMemoryPercent") == 0) {
      filter_minmempct_g = c->values[0].value.number;
      if (filter_minmempct_g < 0.0 || filter_minmempct_g > 100.0) {
        ERROR("mapr_process plugin: MinMemoryPercent out of [0,100] range");
        continue;
      }
    } else if (strcasecmp(c->key, "PID_Directory") == 0) {
      if ((c->values_num != 1)
          || (OCONFIG_TYPE_STRING != c->values[0].type)) {
        ERROR("mapr_process plugin: `PID_Directory' expects exactly "
            "one string argument (got %i).", c->values_num);
        continue;
      }

      if (c->children_num != 0) {
        WARNING("mapr_process plugin: the `PID_Directory' config option "
            "does not expect any child elements -- ignoring "
            "content (%i elements) of the <PID_Directory '%s'> block.",
            c->children_num, c->values[0].value.string);
      }

      int found = 0;
      for (dirlist=directory_list_head_g;dirlist != NULL;dirlist = dirlist->next) {
        // Either you have found the entry or you have reached end of the list
        if (strcmp(dirlist->directoryName, c->values[0].value.string) == 0) {
          WARNING("mapr_process plugin: Found more than one entry for directory name. Ignoring the current entry");
          found = 1;

        }
        if (dirlist->next == NULL) break;
      }

      if (found == 0) {
        directorylist_t *newEntry = (directorylist_t *) malloc(sizeof(directorylist_t));
        memset(newEntry, 0, sizeof(directorylist_t));
        strcpy(newEntry->directoryName,c->values[0].value.string);
        if (newEntry == NULL) {
          ERROR("mapr_process plugin: creating directory list malloc failed.");
          continue;
        }
        if (dirlist == NULL) {
          directory_list_head_g = newEntry;
        } else {
          dirlist->next = newEntry;
        }
        getPids(c->values[0].value.string);
      }
    } else {
      ERROR("mapr_process plugin: The `%s' configuration option is not "
          "understood and will be ignored.", c->key);
      continue;
    }
  }

  return (0);
}

static int ps_init(void) {
  pagesize_g = sysconf(_SC_PAGESIZE);
  clockTicks = sysconf(_SC_CLK_TCK);
  numCores = (uint)sysconf(_SC_NPROCESSORS_ONLN);
  INFO ("pagesize_g = %li; clockTicks = %li; numCores = %d;",
      pagesize_g, clockTicks, numCores);
  return (0);
} /* int ps_init */

/* submit info about specific process (e.g.: memory taken, cpu usage, etc..) */
static void ps_submit_proc_list(procstat_t *ps) {
  value_t values[2];
  value_list_t vl = VALUE_LIST_INIT;

  vl.values = values;
  vl.values_len = 2;
  sstrncpy(vl.host, hostname_g, sizeof(vl.host));
  sstrncpy(vl.plugin, "mapr.process", sizeof(vl.plugin));
  sstrncpy(vl.plugin_instance, ps->processName, sizeof(vl.plugin_instance));

  sstrncpy(vl.type, "vm", sizeof(vl.type));
  vl.values[0].gauge = ps->vmem_size;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "rss", sizeof(vl.type));
  vl.values[0].gauge = ps->vmem_rss;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "data", sizeof(vl.type));
  vl.values[0].gauge = ps->vmem_data;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "stack_size", sizeof(vl.type));
  vl.values[0].gauge = ps->stack_size;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "cpu_time", sizeof(vl.type));
  vl.values[0].derive = ps->cpu_user_counter;
  vl.values[1].derive = ps->cpu_system_counter;
  vl.values_len = 2;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "cpu_percent", sizeof(vl.type));
  vl.values[0].gauge = ps->cpu_percent;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "mem_percent", sizeof(vl.type));
  vl.values[0].gauge = ps->mem_percent;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "page_faults", sizeof(vl.type));
  vl.values[0].derive = ps->vmem_minflt_counter;
  vl.values[1].derive = ps->vmem_majflt_counter;
  vl.values_len = 2;
  plugin_dispatch_values(&vl);

  if ((ps->io_rchar != -1) && (ps->io_wchar != -1)) {
    sstrncpy(vl.type, "disk_octets", sizeof(vl.type));
    vl.values[0].derive = ps->io_rchar;
    vl.values[1].derive = ps->io_wchar;
    vl.values_len = 2;
    plugin_dispatch_values(&vl);
  }

  if ((ps->io_syscr != -1) && (ps->io_syscw != -1)) {
    sstrncpy(vl.type, "disk_ops", sizeof(vl.type));
    vl.values[0].derive = ps->io_syscr;
    vl.values[1].derive = ps->io_syscw;
    vl.values_len = 2;
    plugin_dispatch_values(&vl);
  }

  sstrncpy(vl.type, "context_switch_voluntary", sizeof(vl.type));
  vl.values[0].derive = ps->cswitch_vol;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);

  sstrncpy(vl.type, "context_switch_involuntary", sizeof(vl.type));
  vl.values[0].derive = ps->cswitch_invol;
  vl.values_len = 1;
  plugin_dispatch_values(&vl);


  DEBUG ("name = %s; num_proc = %lu; num_lwp = %lu; "
      "vmem_size = %lu; vmem_rss = %lu; vmem_data = %lu; "
      "vmem_code = %lu; "
      "vmem_minflt_counter = %"PRIi64"; vmem_majflt_counter = %"PRIi64"; "
      "cpu_user_counter = %"PRIi64"; cpu_system_counter = %"PRIi64"; "
      "io_rchar = %"PRIi64"; io_wchar = %"PRIi64"; "
      "io_syscr = %"PRIi64"; io_syscw = %"PRIi64"; "
      "cswitch_vol = %"PRIi64"; cswitch_invol = %"PRIi64"; "
      "cpu_percent = %f; mem_percent = %f; pid = %lu; ppid = %lu; "
      "runtime = %lu secs",
      ps->name, ps->num_proc, ps->num_lwp,
      ps->vmem_size, ps->vmem_rss,
      ps->vmem_data, ps->vmem_code,
      ps->vmem_minflt_counter, ps->vmem_majflt_counter,
      ps->cpu_user_counter, ps->cpu_system_counter,
      ps->io_rchar, ps->io_wchar, ps->io_syscr, ps->io_syscw,
      ps->cswitch_vol, ps->cswitch_invol, ps->cpu_percent,
      ps->mem_percent, ps->pid, ps->ppid, ps->runtime_secs);
} /* void ps_submit_proc_list */

static procstat_t *ps_read_tasks_status (int pid, procstat_t *ps)
{
  char dirname[64];
  DIR *dh;
  char filename[64];
  FILE *fh;
  struct dirent *ent;
  derive_t cswitch_vol = 0;
  derive_t cswitch_invol = 0;
  derive_t cpu_user_counter = 0;
  derive_t cpu_system_counter = 0;
  char buffer[1024];
  char *fields[8];
  int numfields;
  ssize_t status;
  size_t buffer_len;
  char *buffer_ptr;
  size_t name_start_pos;
  size_t name_end_pos;
  char *statFields[64];
  char fields_len;
  char statBuffer[1024];


  ssnprintf (dirname, sizeof (dirname), "/proc/%i/task", pid);

  if ((dh = opendir (dirname)) == NULL)
  {
    DEBUG ("Failed to open directory `%s'", dirname);
    return (NULL);
  }

  while ((ent = readdir (dh)) != NULL)
  {
    char *tpid;

    if (!isdigit ((int) ent->d_name[0]))
      continue;

    tpid = ent->d_name;

    ssnprintf (filename, sizeof (filename), "/proc/%i/task/%s/status", pid, tpid);
    if ((fh = fopen (filename, "r")) == NULL)
    {
      ERROR ("Failed to open file `%s'", filename);
      continue;
    }

    while (fgets (buffer, sizeof(buffer), fh) != NULL)
    {
      derive_t tmp;
      char *endptr;

      if (strncmp (buffer, "voluntary_ctxt_switches", 23) != 0
          && strncmp (buffer, "nonvoluntary_ctxt_switches", 26) != 0)
        continue;

      numfields = strsplit (buffer, fields,
          STATIC_ARRAY_SIZE (fields));

      if (numfields < 2)
        continue;

      errno = 0;
      endptr = NULL;
      tmp = (derive_t) strtoll (fields[1], &endptr, /* base = */10);
      if ((errno == 0) && (endptr != fields[1]))
      {
        if (strncmp (buffer, "voluntary_ctxt_switches", 23) == 0)
        {
          cswitch_vol += tmp;
        }
        else if (strncmp (buffer, "nonvoluntary_ctxt_switches", 26) == 0)
        {
          cswitch_invol += tmp;
        }
      }
    } /* while (fgets) */

    if (fclose (fh))
    {
      char errbuf[1024];
      WARNING ("mapr_process plugin: fclose: %s", sstrerror (errno, errbuf, sizeof (errbuf)));
    }

    // Read /proc/pid/task/taskid/stat file
    ssnprintf (filename, sizeof (filename), "/proc/%i/task/%s/stat", pid, tpid);
    INFO ("mapr_process plugin: Reading task %s file for process %s", tpid, ps->name);
    status = read_file_contents (filename, statBuffer, sizeof(statBuffer) - 1);
    if (status <= 0)
      return (NULL);
    buffer_len = (size_t) status;
    statBuffer[buffer_len] = 0;
    name_start_pos = 0;
    while ((statBuffer[name_start_pos] != '(')
        && (name_start_pos < buffer_len))
      name_start_pos++;

    name_end_pos = buffer_len;
    while ((statBuffer[name_end_pos] != ')')
        && (name_end_pos > 0))
      name_end_pos--;

    /* Either '(' or ')' is not found or they are in the wrong order.
     * Anyway, something weird that shouldn't happen ever. */
    if (name_start_pos >= name_end_pos)
    {
      ERROR ("mapr_process plugin: name_start_pos = %zu >= name_end_pos = %zu",
          name_start_pos, name_end_pos);
      return (NULL);
    }

    if ((buffer_len - name_end_pos) < 2)
      return (NULL);
    buffer_ptr = &statBuffer[name_end_pos + 2];

    // Split the fields
    fields_len = strsplit (buffer_ptr, statFields, STATIC_ARRAY_SIZE (statFields));
    if (fields_len < 22)
    {
      DEBUG ("mapr_process plugin: ps_read_task (pid = %i): for task %s"
          " `%s' has only %i fields..",
          (int) pid, tpid,filename, fields_len);
      return (NULL);
    }

    // Aggregate it for all tasks
    cpu_user_counter = cpu_user_counter + atoll (statFields[11]);
    cpu_system_counter = cpu_system_counter + atoll (statFields[12]);
  }
  closedir (dh);

  ps->cswitch_vol = cswitch_vol;
  ps->cswitch_invol = cswitch_invol;

  // Convert clock ticks to seconds
  INFO ("mapr_process plugin: Before cpu user counter %"PRIi64" for pid %lu for process %s", ps->cpu_user_counter,ps->pid, ps->name);
  ps->cpu_user_counter = ps->cpu_user_counter + cpu_user_counter;
  INFO ("mapr_process plugin: After cpu user counter %"PRIi64" for pid %lu for process %s",ps->cpu_user_counter,ps->pid, ps->name);
  INFO ("mapr_process plugin: Before cpu system counter %"PRIi64" for pid %lu for process %s",ps->cpu_system_counter,ps->pid, ps->name);
  ps->cpu_system_counter = ps->cpu_system_counter + cpu_system_counter;
  INFO ("mapr_process plugin: After cpu system counter %"PRIi64" for pid %lu for process %s",ps->cpu_system_counter,ps->pid, ps->name);

  return (ps);
} /* int *ps_read_tasks_status */

/* Read data from /proc/pid/status */
static procstat_t *ps_read_status (int pid, procstat_t *ps)
{
  FILE *fh;
  char buffer[1024];
  char filename[64];
  unsigned long lib = 0;
  unsigned long exe = 0;
  unsigned long data = 0;
  unsigned long threads = 0;
  char *fields[8];
  int numfields;

  ssnprintf (filename, sizeof (filename), "/proc/%i/status", pid);
  if ((fh = fopen (filename, "r")) == NULL)
    return (NULL);

  while (fgets (buffer, sizeof(buffer), fh) != NULL)
  {
    unsigned long tmp;
    char *endptr;

    if (strncmp (buffer, "Vm", 2) != 0
        && strncmp (buffer, "Threads", 7) != 0)
      continue;

    numfields = strsplit (buffer, fields,
        STATIC_ARRAY_SIZE (fields));

    if (numfields < 2)
      continue;

    errno = 0;
    endptr = NULL;
    tmp = strtoul (fields[1], &endptr, /* base = */10);
    if ((errno == 0) && (endptr != fields[1]))
    {
      if (strncmp (buffer, "VmData", 6) == 0)
      {
        data = tmp;
      }
      else if (strncmp (buffer, "VmLib", 5) == 0)
      {
        lib = tmp;
      }
      else if (strncmp(buffer, "VmExe", 5) == 0)
      {
        exe = tmp;
      }
      else if (strncmp(buffer, "Threads", 7) == 0)
      {
        threads = tmp;
      }
    }
  } /* while (fgets) */

  if (fclose (fh))
  {
    char errbuf[1024];
    WARNING ("processes: fclose: %s",
        sstrerror (errno, errbuf, sizeof (errbuf)));
  }

  ps->vmem_data = data;
  ps->vmem_code = (exe + lib);
  if (threads != 0)
    ps->num_lwp = threads;

  return (ps);
} /* procstat_t *ps_read_vmem */

static procstat_t *ps_read_io (int pid, procstat_t *ps)
{
  FILE *fh;
  char buffer[1024];
  char filename[64];

  char *fields[8];
  int numfields;

  ssnprintf (filename, sizeof (filename), "/proc/%i/io", pid);
  if ((fh = fopen (filename, "r")) == NULL)
    return (NULL);

  while (fgets (buffer, sizeof (buffer), fh) != NULL)
  {
    derive_t *val = NULL;
    long long tmp;
    char *endptr;

    if (strncasecmp (buffer, "rchar:", 6) == 0)
      val = &(ps->io_rchar);
    else if (strncasecmp (buffer, "wchar:", 6) == 0)
      val = &(ps->io_wchar);
    else if (strncasecmp (buffer, "syscr:", 6) == 0)
      val = &(ps->io_syscr);
    else if (strncasecmp (buffer, "syscw:", 6) == 0)
      val = &(ps->io_syscw);
    else
      continue;

    numfields = strsplit (buffer, fields,
        STATIC_ARRAY_SIZE (fields));

    if (numfields < 2)
      continue;

    errno = 0;
    endptr = NULL;
    tmp = strtoll (fields[1], &endptr, /* base = */10);
    if ((errno != 0) || (endptr == fields[1]))
      *val = -1;
    else
      *val = (derive_t) tmp;
  } /* while (fgets) */

  if (fclose (fh))
  {
    char errbuf[1024];
    WARNING ("processes: fclose: %s",
        sstrerror (errno, errbuf, sizeof (errbuf)));
  }

  return (ps);
} /* procstat_t *ps_read_io */

static sysstat_t *ps_read_sys_stat(void)
{
  char buffer[1024];
  char name[32];
  unsigned long long sys_cpu_user_counter;
  unsigned long long sys_cpu_user_nice_counter;
  unsigned long long sys_cpu_system_counter;
  unsigned long long sys_cpu_idle_counter;
  unsigned long long sys_cpu_iowait_counter;
  unsigned long long sys_cpu_irq_counter;
  unsigned long long sys_cpu_softirq_counter;
  unsigned long long sys_cpu_steal_counter;
  unsigned long long sys_cpu_guest_counter;
  unsigned long long sys_tot_phys_mem;
  struct sysinfo si;
  sysstat_t *ss;

  read_file_contents("/proc/stat", buffer, sizeof(buffer));
  sscanf(buffer, "%s %llu %llu %llu %llu %llu %llu %llu %llu %llu", name,
      &sys_cpu_user_counter, &sys_cpu_user_nice_counter,
      &sys_cpu_system_counter, &sys_cpu_idle_counter, &sys_cpu_iowait_counter, &sys_cpu_irq_counter, &sys_cpu_softirq_counter, &sys_cpu_steal_counter, &sys_cpu_guest_counter);
  if (strcmp(name, "cpu") != 0) {
    ERROR ("processes plugin: unexpected string in /proc/stat");
    return NULL;
  }

  if (sysinfo(&si) < 0) {
    ERROR ("processes plugin: cannot obtain system info via sysinfo()");
    return NULL;
  }

  sys_tot_phys_mem = si.totalram * si.mem_unit;

  ss = (sysstat_t *)malloc(sizeof(sysstat_t));
  if (ss == NULL) {
    ERROR ("processes plugin: error allocating memory");
    return NULL;
  }
  ss->sys_cpu_user_counter = sys_cpu_user_counter;
  ss->sys_cpu_system_counter = sys_cpu_system_counter;
  ss->sys_cpu_tot_time_counter = sys_cpu_user_counter +
      sys_cpu_user_nice_counter + sys_cpu_system_counter +
      sys_cpu_idle_counter + sys_cpu_iowait_counter + sys_cpu_irq_counter + sys_cpu_softirq_counter + sys_cpu_steal_counter + sys_cpu_guest_counter;
  ss->sys_boot_time_secs = si.uptime;
  ss->sys_tot_phys_mem = sys_tot_phys_mem;
  ss->sys_boot_time_secs = time(NULL) - ss->sys_boot_time_secs;
  INFO ("%s sys u:%llu n:%llu s:%llu i:%llu physmem: %llu\n",
      name, sys_cpu_user_counter, sys_cpu_user_nice_counter,
      sys_cpu_system_counter, sys_cpu_idle_counter, sys_tot_phys_mem);
  return ss;
}

int ps_read_process (int pid, procstat_t *ps, char *state)
{
  char filename[64];
  char buffer[1024];

  char *fields[64];
  char fields_len;

  size_t buffer_len;

  char *buffer_ptr;
  size_t name_start_pos;
  size_t name_end_pos;
  size_t name_len;

  derive_t cpu_user_counter;
  derive_t cpu_system_counter;
  derive_t cpu_child_user_counter;
  derive_t cpu_child_system_counter;

  long long unsigned vmem_size;
  long long unsigned vmem_rss;
  long long unsigned stack_size;

  ssize_t status;

  memset (ps, 0, sizeof (procstat_t));

  ssnprintf (filename, sizeof (filename), "/proc/%i/stat", pid);

  status = read_file_contents (filename, buffer, sizeof(buffer) - 1);
  if (status <= 0)
    return (-1);
  buffer_len = (size_t) status;
  buffer[buffer_len] = 0;

  /* The name of the process is enclosed in parens. Since the name can
   * contain parens itself, spaces, numbers and pretty much everything
   * else, use these to determine the process name. We don't use
   * strchr(3) and strrchr(3) to avoid pointer arithmetic which would
   * otherwise be required to determine name_len. */
  name_start_pos = 0;
  while ((buffer[name_start_pos] != '(')
      && (name_start_pos < buffer_len))
    name_start_pos++;

  name_end_pos = buffer_len;
  while ((buffer[name_end_pos] != ')')
      && (name_end_pos > 0))
    name_end_pos--;

  /* Either '(' or ')' is not found or they are in the wrong order.
   * Anyway, something weird that shouldn't happen ever. */
  if (name_start_pos >= name_end_pos)
  {
    ERROR ("mapr_process plugin: name_start_pos = %zu >= name_end_pos = %zu",
        name_start_pos, name_end_pos);
    return (-1);
  }

  name_len = (name_end_pos - name_start_pos) - 1;
  if (name_len >= sizeof (ps->name))
    name_len = sizeof (ps->name) - 1;

  sstrncpy (ps->name, &buffer[name_start_pos + 1], name_len + 1);

  if ((buffer_len - name_end_pos) < 2)
    return (-1);
  buffer_ptr = &buffer[name_end_pos + 2];

  fields_len = strsplit (buffer_ptr, fields, STATIC_ARRAY_SIZE (fields));
  if (fields_len < 22)
  {
    DEBUG ("mapr_process plugin: ps_read_process (pid = %i):"
        " `%s' has only %i fields..",
        (int) pid, filename, fields_len);
    return (-1);
  }

  *state = fields[0][0];

  if (*state == 'Z')
  {
    ps->num_lwp = 0;
    ps->num_proc = 0;
  }
  else
  {
    ps->num_lwp = strtoul (fields[17], /* endptr = */NULL, /* base = */10);
    if ((ps_read_status(pid, ps)) == NULL)
    {
      /* No VMem data */
      ps->vmem_data = -1;
      ps->vmem_code = -1;
      DEBUG("ps_read_process: did not get vmem data for pid %i",pid);
    }
    if (ps->num_lwp <= 0)
      ps->num_lwp = 1;
    ps->num_proc = 1;
  }

  /* Leave the rest at zero if this is only a zombi */
  if (ps->num_proc == 0)
  {
    DEBUG ("processes plugin: This is only a zombi: pid = %i; "
        "name = %s;", pid, ps->name);
    return (0);
  }

  cpu_user_counter = atoll (fields[11]);
  cpu_system_counter = atoll (fields[12]);
  cpu_child_user_counter = 0;
  cpu_child_system_counter = 0;

  //cpu_child_user_counter = atoll (fields[13]);
  //cpu_child_system_counter = atoll (fields[14]);

  vmem_size = atoll (fields[20]);
  vmem_rss = atoll (fields[21]);
  ps->vmem_minflt_counter = atol (fields[7]);
  ps->vmem_majflt_counter = atol (fields[9]);
  ps->pid = pid;
  ps->ppid = atol (fields[1]);
  ps->starttime_secs = atoll (fields[19]) / clockTicks;

  {
    unsigned long long stack_start = atoll (fields[25]);
    unsigned long long stack_ptr = atoll (fields[26]);

    stack_size = (stack_start > stack_ptr) ? stack_start - stack_ptr : stack_ptr - stack_start;
  }

  cpu_user_counter = cpu_user_counter + cpu_child_user_counter;
  cpu_system_counter = cpu_system_counter + cpu_child_system_counter;
  vmem_rss = vmem_rss * pagesize_g;

  ps->cpu_user_counter = cpu_user_counter;
  ps->cpu_system_counter = cpu_system_counter;
  ps->vmem_size = (unsigned long) vmem_size;
  ps->vmem_rss = (unsigned long) vmem_rss;
  ps->stack_size = (unsigned long) stack_size;

  if ( (ps_read_io (pid, ps)) == NULL)
  {
    /* no io data */
    ps->io_rchar = -1;
    ps->io_wchar = -1;
    ps->io_syscr = -1;
    ps->io_syscw = -1;

    DEBUG("ps_read_process: not get io data for pid %i",pid);
  }

  if ( (ps_read_tasks_status(pid, ps)) == NULL)
  {
    ps->cswitch_vol = -1;
    ps->cswitch_invol = -1;

    DEBUG("ps_read_tasks_status: not get context "
        "switch data for pid %i",pid);
  }


  /* success */
  return (0);
} /* int ps_read_process (...) */


static _Bool config_threshold_exceeded(procstat_t *ps)
{
  if (ps->mem_percent >= filter_minmempct_g)
    return 1;
  if (ps->cpu_percent >= filter_mincpupct_g)
    return 1;
  return 0;
}

static void ps_find_cpu_delta(procstat_t *ps,  double *out_userd,  double *out_sysd)
{
  procstat_t *ps_ptr;
  for (ps_ptr=prev_proc_list_head_g; ps_ptr!=NULL; ps_ptr=ps_ptr->next) {
    if (ps_ptr->pid == ps->pid)
      break;
  }

  if (ps_ptr) {
    INFO ("Current cpu user counter %"PRIi64" , previous counter %"PRIi64" for pid %lu for process %s",ps->cpu_user_counter, ps_ptr->cpu_user_counter,ps->pid, ps->name);
    *out_userd = ps->cpu_user_counter - ps_ptr->cpu_user_counter;
    INFO ("Current cpu system counter %"PRIi64" , previous counter %"PRIi64" for pid %lu for process %s",ps->cpu_system_counter, ps_ptr->cpu_system_counter,ps->pid, ps->name);
    *out_sysd = ps->cpu_system_counter - ps_ptr->cpu_system_counter;
  }
  else {
    *out_userd = *out_sysd  = 0ULL;
  }
}


static void ps_calc_mem_percent(sysstat_t *ss, procstat_t *ps)
{
  /* +0.5 to round off to nearest int */
  ps->mem_percent = (ps->vmem_rss*100.0 / ss->sys_tot_phys_mem);
}

static void ps_calc_runtime(sysstat_t *ss, procstat_t *ps)
{
  ps->runtime_secs = time(NULL) - (ps->starttime_secs + ss->sys_boot_time_secs);
}

static void ps_calc_cpu_percent(sysstat_t *ss, sysstat_t *prev_ss, procstat_t *ps)
{
  if (ss && prev_ss) {
    INFO("Previous system stats for cpu percent for pid %lu for process %s : %ld, %ld",ps->pid, ps->name,prev_ss->sys_cpu_system_counter, prev_ss->sys_cpu_tot_time_counter);
    INFO("Current system stats for cpu percent for pid %lu for process %s : %ld, %ld",ps->pid, ps->name,ss->sys_cpu_system_counter, ss->sys_cpu_tot_time_counter);
    double ps_cpu_user_delta, ps_cpu_system_delta;
    double ss_cpu_tot_time_delta;
    static struct timeval oldtimev;
    struct timeval timev;
    struct timezone timez;
    float et;

    gettimeofday(&timev, &timez);
    et = (timev.tv_sec - oldtimev.tv_sec) + (float)((timev.tv_usec - oldtimev.tv_usec) / 1000000.0);
    oldtimev.tv_sec = timev.tv_sec;
    oldtimev.tv_usec = timev.tv_usec;

    //unsigned long ss_cpu_boot_time_delta;
    double cpu_percent;
    ps_find_cpu_delta(ps, &ps_cpu_user_delta, &ps_cpu_system_delta);
    ss_cpu_tot_time_delta = ss->sys_cpu_tot_time_counter - prev_ss->sys_cpu_tot_time_counter;
    cpu_percent = (ps_cpu_system_delta + ps_cpu_user_delta) * 100.0 * numCores / ss_cpu_tot_time_delta;
    //cpu_percent = (ps_cpu_system_delta + ps_cpu_user_delta) * 100.0 / et ;

    INFO ("%s proc with %lu pid delta: u: %f, s: %f, tot: %f, percent: %f\n", ps->name, ps->pid,ps_cpu_user_delta, ps_cpu_system_delta, ss_cpu_tot_time_delta,cpu_percent);
    ps->cpu_percent = cpu_percent;
  }
}

/* do actual readings from kernel */
static int ps_read(void) {
  int status;
  procstat_t ps;
  procstat_t *ps_ptr;
  sysstat_t *ss;
  char state;
  directorylist_t *dirlist;
  static sysstat_t *prev_ss=NULL;

  /*
   * Read /proc file and get the number of processes and
   */
  //numOfProcesses = getProcesses();

  /*
   * Make the process tree
   */
  //MakeTree();

  /*
   * Check if the PIDs changed
   */

  for (dirlist=directory_list_head_g;dirlist != NULL;dirlist = dirlist->next) {
    getPids(dirlist->directoryName);
  }

  ss = ps_read_sys_stat();

  for (ps_ptr = list_head_g; ps_ptr != NULL; ps_ptr = ps_ptr->next) {
    status = ps_read_process (ps_ptr->pid, &ps, &state);
    if (status != 0) {
      DEBUG ("ps_read_process failed: %i", status);
      continue;
    }
    ps_calc_runtime(ss, &ps);
    ps_calc_mem_percent(ss, &ps);
    ps_calc_cpu_percent(ss, prev_ss, &ps);
    sstrncpy(ps.processName, ps_ptr->processName, sizeof(ps.processName));

    // Store the per process metrics so you can use them to compute deltas
    ps_proc_list_prepend(&ps);
  }

  for (ps_ptr = proc_list_head_g; ps_ptr != NULL; ps_ptr = ps_ptr->next) {
    if (config_threshold_exceeded(ps_ptr))
      ps_submit_proc_list (ps_ptr);
  }
  ps_proc_list_reset(&prev_proc_list_head_g);
  prev_proc_list_head_g = proc_list_head_g;
  if (prev_ss)
    free(prev_ss);
  prev_ss = ss;
  INFO("Current system stats: %ld, %ld",prev_ss->sys_cpu_system_counter, prev_ss->sys_cpu_tot_time_counter);
  proc_list_head_g = NULL;

  free(P);
  return (0);
} /* int ps_read */

void module_register(void) {
  plugin_register_complex_config("mapr_process", ps_config);
  plugin_register_init("mapr_process", ps_init);
  plugin_register_read("mapr_process", ps_read);
} /* void module_register */
